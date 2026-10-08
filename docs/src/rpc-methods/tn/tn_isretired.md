# tn\_isRetired

Returns `true` if the validator is permanently retired, read from the `ConsensusRegistry` contract at the node's canonical tip. A validator retires when it unstakes, when governance burns its ConsensusNFT, or when slashing takes its stake to zero; a retired address can never rejoin. An address that still holds a ConsensusNFT, or that was never a validator, returns `false`. The zero address reverts with the registry's `InvalidTokenId(uint256)` error.

#### Parameters

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`Boolean` - `true` if the validator is retired, otherwise `false`.

A revert is reported as `eth_call` reports one: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_isRetired","params":["0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": false
}
```
