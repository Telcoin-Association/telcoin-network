# tn\_getCurrentStakeVersion

Returns the current stake config version, read from the `ConsensusRegistry` contract at the node's canonical tip. This is the stake version the current epoch was stamped with when it began, the same `stakeVersion` that [tn\_getCurrentEpochInfo](tn_getcurrentepochinfo.md) reports. A version governance creates with `upgradeStakeVersion()` takes effect at the next epoch, so until then this method still returns the previous version. [How Staking Works](../../staking/how-staking-works.md) describes stake versions.

#### Parameters

`None`

#### Returns

`Number` - The stake version, a JSON number from `0` to `255`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getCurrentStakeVersion","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": 0
}
```
