# tn\_proofOfPossessionMessage

Returns the BLS12-381 proof-of-possession message a validator signs: `intentPrefix(3) || compressedBlsPubkey(96) || validatorAddress(20)`, 119 bytes in all. The validator's signature over this message is the proof of possession it submits when it stakes; see [How to Stake](../../staking/how-to-stake.md). The node builds the message itself with the same routine validators sign with, which the `ConsensusRegistry` contract reproduces byte for byte on-chain, so the method reads no chain state and signs or submits nothing.

The method expects the 96-byte compressed BLS public key. The 192-byte uncompressed encoding is also accepted and normalized to the compressed form, so both yield the same message.

#### Parameters

`DATA`, 96 or 192 bytes - The validator's BLS12-381 public key, compressed (96 bytes) or uncompressed (192 bytes).

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`DATA`, 119 bytes - The message to sign: the 3-byte Telcoin Network proof-of-possession intent, the compressed public key, and the address.

A key of any other length fails with error `-32602` and the message `invalid BLS pubkey: expected 96 bytes (compressed) or 192 bytes (uncompressed), got <length>`. A key of the right length that does not decode to a valid BLS12-381 public key also fails with `-32602`, with a message that starts with `invalid 96-byte compressed BLS pubkey`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_proofOfPossessionMessage","params":["0xb1f5f47e63566357362e2157f1b6860df78c08e98f7f5df8e0e2cf837cdea6420c0698dc123f187ed170acfe5c138bf11202b69c3ce2c4f5c2de7fa58d993931164fc3011118c62f0fb1b9cf723f1e55a410a9f77e8cd41576c9e4f8d0adf8ba", "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x000000b1f5f47e63566357362e2157f1b6860df78c08e98f7f5df8e0e2cf837cdea6420c0698dc123f187ed170acfe5c138bf11202b69c3ce2c4f5c2de7fa58d993931164fc3011118c62f0fb1b9cf723f1e55a410a9f77e8cd41576c9e4f8d0adf8ba3518b301b86ceb53b5a3dff62e55cd43ef59d024"
}
```
