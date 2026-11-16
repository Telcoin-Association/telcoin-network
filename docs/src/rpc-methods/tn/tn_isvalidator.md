# tn\_isValidator

Returns `true` if the BLS public key belongs to a known validator, read from the `ConsensusRegistry` contract at the node's canonical tip. A key is known when a validator registered it by staking and that validator has not retired; the validator's status is otherwise not checked, so a `Staked` or `Exited` validator's key also returns `true`. The input is never rejected: a key that is not exactly 96 bytes, or that no validator registered, returns `false`.

#### Parameters

`DATA`, 96 bytes - The compressed BLS12-381 public key. The 192-byte uncompressed encoding is not accepted here and returns `false`.

#### Returns

`Boolean` - `true` if a validator that has not retired registered this key, otherwise `false`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_isValidator","params":["0xb1f5f47e63566357362e2157f1b6860df78c08e98f7f5df8e0e2cf837cdea6420c0698dc123f187ed170acfe5c138bf11202b69c3ce2c4f5c2de7fa58d993931164fc3011118c62f0fb1b9cf723f1e55a410a9f77e8cd41576c9e4f8d0adf8ba"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": true
}
```
