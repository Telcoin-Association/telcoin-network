---
description: Returns an array of all logs matching a given filter object.
---

# eth\_getLogs

#### Parameters

`Object` - The filter options:

* `fromBlock`: `QUANTITY|TAG` - (optional, default: `"latest"`) Hexadecimal block number, or `"latest"` for the last mined block, `"safe"` for the latest safe block, `"finalized"` for the latest finalized block, or `"earliest"` for not yet mined transactions.
* `toBlock`: `QUANTITY|TAG` - (optional, default: `"latest"`) Hexadecimal block number, or `"latest"` for the last mined block, `"safe"` for the latest safe block, `"finalized"` for the latest finalized block, or `"earliest"` for not yet mined transactions.
* `address`: `DATA|Array`, 20 Bytes - (optional) Contract address or a list of addresses from which logs should originate.
* `topics`: `Array of DATA`, - (optional) Array of 32 Bytes `DATA` topics. Topics are order-dependent. Each topic can also be an array of DATA with "or" options.
* `blockhash`: `DATA`, 32 Bytes - (optional, **future**) With the addition of EIP-234, `blockHash` will be a new filter option which restricts the logs returned to the single block with the 32-byte hash `blockHash`. Using `blockHash` is equivalent to `fromBlock` = `toBlock` = the block number with hash `blockHash`. If `blockHash` is present in the filter criteria, then neither `fromBlock` nor `toBlock` are allowed.

#### Returns

`Array` - Array of log objects, or an empty array if nothing has changed since last poll.

For filters created with `eth_newBlockFilter` the return are block hashes (`DATA`, 32 Bytes), e.g. `["0x3454645634534..."]`.

For filters created with `eth_newPendingTransactionFilter` the return are transaction hashes (`DATA`, 32 Bytes), e.g. `["0x6345343454645..."]`.

For filters created with `eth_newFilter` logs are objects with following params:

* `removed`: `TAG` - `true` when the log was removed, due to a chain reorganization. `false` if its a valid log.
* `logIndex`: `QUANTITY` - Hexadecimal log index position in the block. `null` when its pending log.
* `transactionIndex`: `QUANTITY` - Hexadecimal of the transactions index position log was created from. `null` when its pending log.
* `transactionHash`: `DATA`, 32 Bytes - Hash of the transactions this log was created from. `null` when its pending log.
* `blockHash`: `DATA`, 32 Bytes - Hash of the block where this log was in. `null` when its pending. `null` when its pending log.
* `blockNumber`: `QUANTITY` - Hexadecimal of the block number where this log was in. `null` when its pending. `null` when its pending log.
* `address`: `DATA`, 20 Bytes - Address from which this log originated.
* `data`: `DATA` - Contains zero or more 32 Bytes non-indexed arguments of the log.
* `topics`: `Array of DATA` - Array of 0 to 4 32 Bytes `DATA` of indexed log arguments. (In _solidity_: The first topic is the _hash_ of the signature of the event (e.g. `Deposit(address,bytes32,uint256)`), except you declared the event with the `anonymous` specifier.)

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getLogs","params":[{
          "fromBlock": "0x608EED",
          "toBlock": "0x608EEE",
          "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
          ]
        }],"id":74}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 74,
  "result": [
    {
      "address": "0xa2c07c15173c183771ffad40c2e972f97e0bce64",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000003da87b1c3743bd2da60df2ef1bc6f26ef9da6086",
        "0x000000000000000000000000e2005c7718f3849f39c65b8ea93c7a7030aec463"
      ],
      "data": "0x00000000000000000000000000000000000000000000000001a6fd80655b3000",
      "blockHash": "0x5fb9fff1f0f914360abfefb0eac9c7d4fc733007c62c7caf1860d4052776b7d7",
      "blockNumber": "0x608eed",
      "transactionHash": "0x7e7e6d8a61ff36ab337b42b6ed2edf34761f928a110aba857aae97121d35371b",
      "transactionIndex": "0x0",
      "logIndex": "0x0",
      "removed": false
    },
    {
      "address": "0x80d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x000000000000000000000000e2005c7718f3849f39c65b8ea93c7a7030aec463",
        "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3"
      ],
      "data": "0x0000000000000000000000000000000000000000000000027215848ae3242d0b",
      "blockHash": "0x5fb9fff1f0f914360abfefb0eac9c7d4fc733007c62c7caf1860d4052776b7d7",
      "blockNumber": "0x608eed",
      "transactionHash": "0x7e7e6d8a61ff36ab337b42b6ed2edf34761f928a110aba857aae97121d35371b",
      "transactionIndex": "0x0",
      "logIndex": "0x1",
      "removed": false
    },
    {
      "address": "0x80d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3",
        "0x0000000000000000000000000000000000000000000000000000000000000000"
      ],
      "data": "0x0000000000000000000000000000000000000000000000027215848ae3242d0b",
      "blockHash": "0x5fb9fff1f0f914360abfefb0eac9c7d4fc733007c62c7caf1860d4052776b7d7",
      "blockNumber": "0x608eed",
      "transactionHash": "0x7e7e6d8a61ff36ab337b42b6ed2edf34761f928a110aba857aae97121d35371b",
      "transactionIndex": "0x0",
      "logIndex": "0x4",
      "removed": false
    }
  ]
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getlogs)
