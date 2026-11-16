# tn\_isDelegated

Returns `true` if the validator's stake originates from a delegator, read from the `ConsensusRegistry` contract at the node's canonical tip. A validator is delegated when a delegator staked on its behalf with `delegateStake` (see [Delegated Staking](../../staking/how-to-stake.md#delegated-staking-institutional-onboarding)); the delegator, not the validator, then receives the stake and rewards when the validator unstakes. An address with no delegation, including one that was never a validator, returns `false`.

#### Parameters

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`Boolean` - `true` if the registry records a delegator for this validator, otherwise `false`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_isDelegated","params":["0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": false
}
```
