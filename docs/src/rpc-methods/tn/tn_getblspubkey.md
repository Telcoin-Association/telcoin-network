# tn\_getBlsPubkey

Returns the BLS public key for a given validator address, read from the `ConsensusRegistry` contract at the node's canonical tip. The key is the compressed BLS12-381 public key the validator registered when it staked. An address with no registered key reverts with the registry's `BlsPubkeyNotFound(address)` error.

#### Parameters

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`DATA`, 96 bytes - The validator's compressed BLS12-381 public key.

A revert is reported as `eth_call` reports one: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getBlsPubkey","params":["0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0xb1f5f47e63566357362e2157f1b6860df78c08e98f7f5df8e0e2cf837cdea6420c0698dc123f187ed170acfe5c138bf11202b69c3ce2c4f5c2de7fa58d993931164fc3011118c62f0fb1b9cf723f1e55a410a9f77e8cd41576c9e4f8d0adf8ba"
}
```
