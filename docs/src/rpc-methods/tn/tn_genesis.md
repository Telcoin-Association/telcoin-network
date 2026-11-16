# tn\_genesis

Returns the genesis the node's chain specification was built from: the chain configuration and the initial state, in the layout of an Ethereum genesis file. The initial state includes Telcoin Network's system contracts, such as the `ConsensusRegistry` at `0x07E17e17E17e17E17e17E17E17E17e17e17E17e1`, with their bytecode and storage, so the response is large (about 140 KB on adiri).

#### Parameters

`None`

#### Returns

`Object` - The genesis object:

* `config`: `Object` - The chain configuration: `chainId` and the block number or timestamp at which each Ethereum fork activates.
* `nonce`: `QUANTITY` - The genesis header nonce.
* `timestamp`: `QUANTITY` - The genesis header timestamp, in seconds since the Unix epoch.
* `extraData`: `DATA` - The genesis header extra data.
* `gasLimit`: `QUANTITY` - The genesis header gas limit.
* `difficulty`: `QUANTITY` - The genesis header difficulty.
* `mixHash`: `DATA`, 32 Bytes - The genesis header mix hash.
* `coinbase`: `DATA`, 20 bytes - The genesis header beneficiary.
* `alloc`: `Object` - The initial accounts, keyed by address. Each account has `balance` (`QUANTITY`, in wei) and, when the genesis sets them, `nonce` (`QUANTITY`), `code` (`DATA`) and `storage` (`Object` mapping 32-byte slots to 32-byte values).
* `baseFeePerGas`: `QUANTITY` - The genesis header base fee. Present only when the genesis sets it, as are `excessBlobGas`, `blobGasUsed`, `number` and `parentHash`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_genesis","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "config": {
      "chainId": 2017,
      "homesteadBlock": 0,
      "daoForkSupport": false,
      "eip150Block": 0,
      "eip155Block": 0,
      "eip158Block": 0,
      "byzantiumBlock": 0,
      "constantinopleBlock": 0,
      "petersburgBlock": 0,
      "istanbulBlock": 0,
      "berlinBlock": 0,
      "londonBlock": 0,
      "shanghaiTime": 0,
      "cancunTime": 0,
      "pragueTime": 0,
      "terminalTotalDifficulty": 0,
      "terminalTotalDifficultyPassed": true
    },
    "nonce": "0x0",
    "timestamp": "0x69fceea7",
    "extraData": "0x",
    "gasLimit": "0x1c9c380",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
      "0x00000000000000000000000000000000000007e1": {
        "nonce": "0x0",
        "balance": "0x0",
        "code": "0xfe"
      },
      "0x0407c96c91afe937c50058fa870a4a745fa9142e": {
        "balance": "0xc9f2c9cd04674edea40000000"
      }
      // ... 35 more accounts, including the system contracts
    },
    "baseFeePerGas": "0x7"
  }
}
```
