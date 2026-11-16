# tn\_getRewards

Returns the claimable rewards accrued for a given validator address, read from the `ConsensusRegistry` contract at the node's canonical tip. The registry counts as rewards whatever the validator's balance holds above the stake amount of its stake version, so the value is `0x0` before any rewards accrue and for an address that was never a validator. [tn\_getBalanceBreakdown](tn_getbalancebreakdown.md) returns the balance and stake amount this is computed from; [How Staking Works](../../staking/how-staking-works.md) explains how rewards are distributed.

#### Parameters

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`QUANTITY` - Hexadecimal of the accrued rewards in wei.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getRewards","params":["0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x2f78093f3eab2752dfb99" // about 3,586,648 TEL
}
```
