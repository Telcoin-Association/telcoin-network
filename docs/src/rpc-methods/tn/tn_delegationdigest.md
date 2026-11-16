# tn\_delegationDigest

Returns the EIP-712 digest a validator signs to accept a delegation. The node computes it by calling the `ConsensusRegistry` contract's `delegationDigest` view at the canonical tip; nothing is signed or submitted. The validator signs the digest, and the delegator passes that signature to `delegateStake` (see [Delegated Staking](../../staking/how-to-stake.md#delegated-staking-institutional-onboarding)).

Besides the four parameters, the digest commits to the current epoch's stake version and to the validator's delegation nonce. If the stake version changes at an epoch boundary, or the validator advances its nonce with `increaseNonce()`, before the delegation is submitted, the signature no longer verifies and a new digest is needed.

#### Parameters

`DATA`, 96 bytes - The validator's compressed BLS12-381 public key. Any other length, including the 192-byte uncompressed encoding, reverts with the registry's `InvalidBLSPubkey()` error.

`DATA`, 20 bytes - The validator's execution address. It must hold a ConsensusNFT; otherwise the call reverts with the registry's `InvalidTokenId(uint256)` error.

`DATA`, 20 bytes - The delegator's address: the account that will send the `delegateStake` transaction.

`QUANTITY` - The deadline, a Unix timestamp in seconds. `delegateStake` rejects the signature once the block timestamp is past it. A JSON number is also accepted.

#### Returns

`DATA`, 32 bytes - The EIP-712 digest for the validator to sign.

A revert is reported as `eth_call` reports one: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_delegationDigest","params":["0xb1f5f47e63566357362e2157f1b6860df78c08e98f7f5df8e0e2cf837cdea6420c0698dc123f187ed170acfe5c138bf11202b69c3ce2c4f5c2de7fa58d993931164fc3011118c62f0fb1b9cf723f1e55a410a9f77e8cd41576c9e4f8d0adf8ba", "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024", "0x1111111111111111111111111111111111111111", "0x6b49d200"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x556d539e57d044188fc1d7f4acf049df9037f59fb738985df77475199fb2fb28"
}
```
