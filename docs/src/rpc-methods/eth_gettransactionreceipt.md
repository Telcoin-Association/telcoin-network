---
description: Returns the receipt of a transaction by transaction hash.
---

# eth\_getTransactionReceipt

#### Parameters

`DATA`, 32 Bytes - Hash of a transaction

#### Returns

&#x20;`Object` - A transaction receipt object, or `null` when no receipt was found:

* `transactionHash`: `DATA`, 32 Bytes - Hash of the transaction.
* `transactionIndex`: `QUANTITY` - Hexadecimal of the transactions index position in the block.
* `blockHash`: `DATA`, 32 Bytes - Hash of the block where this transaction was in.
* `blockNumber`: `QUANTITY` - Hexadecimal block number where this transaction was in.
* `from`: `DATA`, 20 Bytes - Address of the sender.
* `to`: `DATA`, 20 Bytes - Address of the receiver. null when its a contract creation transaction.
* `cumulativeGasUsed` : `QUANTITY` - Hexadecimal of the total amount of gas used when this transaction was executed in the block.
* `effectiveGasPrice` : `QUANTITY` - Hexadecimal of the sum of the base fee and tip paid per unit of gas.
* `gasUsed` : `QUANTITY` - Hexadecimal of the amount of gas used by this specific transaction alone.
* `contractAddress` : `DATA`, 20 Bytes - The contract address created, if the transaction was a contract creation, otherwise `null`.
* `logs`: `Array` - Array of log objects, which this transaction generated.
* `logsBloom`: `DATA`, 256 Bytes - Bloom filter for light clients to quickly retrieve related logs.
* `type`: `QUANTITY` - Hexadecimal of the transaction type, `0x0` for legacy transactions, `0x1` for access list types, `0x2` for dynamic fees.

It also returns _either_ :

* `root` : `DATA` 32 bytes of post-transaction stateroot (pre Byzantium)
* `status`: `QUANTITY` either `0x1`(success) or `0x0` (failure)

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": {
    "transactionHash": "0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d",
    "transactionIndex": "0x0",
    "blockHash": "0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb",
    "blockNumber": "0x6038c1",
    "cumulativeGasUsed": "0x1a771",
    "gasUsed": "0x1a771",
    "effectiveGasPrice": "0xb2d05e07",
    "from": "0x3da87b1c3743bd2da60df2ef1bc6f26ef9da6086",
    "to": "0x5eb15fedcca9cb9c5424167e329fd56905ffe0e3",
    "contractAddress": null,
    "logs": [
      {
        "address": "0x80d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5",
        "topics": [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x0000000000000000000000000000000000000000000000000000000000000000",
          "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3"
        ],
        "data": "0x0000000000000000000000000000000000000000000000000121782564b00000",
        "blockHash": "0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb",
        "blockNumber": "0x6038c1",
        "transactionHash": "0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d",
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "removed": false
      },
      {
        "address": "0x80d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5",
        "topics": [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3",
          "0x000000000000000000000000e2005c7718f3849f39c65b8ea93c7a7030aec463"
        ],
        "data": "0x0000000000000000000000000000000000000000000000000121782564b00000",
        "blockHash": "0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb",
        "blockNumber": "0x6038c1",
        "transactionHash": "0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d",
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "removed": false
      },
      {
        "address": "0xa2c07c15173c183771ffad40c2e972f97e0bce64",
        "topics": [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x000000000000000000000000e2005c7718f3849f39c65b8ea93c7a7030aec463",
          "0x0000000000000000000000003da87b1c3743bd2da60df2ef1bc6f26ef9da6086"
        ],
        "data": "0x0000000000000000000000000000000000000000000000000000bf80c3c72242",
        "blockHash": "0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb",
        "blockNumber": "0x6038c1",
        "transactionHash": "0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d",
        "transactionIndex": "0x0",
        "logIndex": "0x2",
        "removed": false
      },
      {
        "address": "0xe2005c7718f3849f39c65b8ea93c7a7030aec463",
        "topics": [
          "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
        ],
        "data": "0x0000000000000000000000000000000000000000000000d4b594d815912d0b3e0000000000000000000000000000000000000000000000008d2445e5c1532614",
        "blockHash": "0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb",
        "blockNumber": "0x6038c1",
        "transactionHash": "0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d",
        "transactionIndex": "0x0",
        "logIndex": "0x3",
        "removed": false
      },
      {
        "address": "0xe2005c7718f3849f39c65b8ea93c7a7030aec463",
        "topics": [
          "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
          "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3",
          "0x0000000000000000000000003da87b1c3743bd2da60df2ef1bc6f26ef9da6086"
        ],
        "data": "0x0000000000000000000000000000000000000000000000000121782564b00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000bf80c3c72242",
        "blockHash": "0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb",
        "blockNumber": "0x6038c1",
        "transactionHash": "0xbd3a14fcf09f3f9e5ca951f2bfedbc7237fe1cd0d68673231f1333eed801830d",
        "transactionIndex": "0x0",
        "logIndex": "0x4",
        "removed": false
      }
    ],
    "logsBloom": "0x00200000040000000000000280000028000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000008000000200000000000000000000000000000000400000000020000000010000002000800000000000000000000000010000000400000000000000000000000000000000000000000000000080000004000000000000100000000000000000000000040200100000000000000000020000000000000000002000000000000200000000000000000000000001000000000000020000080000000020000000000000000000000000000000000000001000000000000",
    "status": "0x1",
    "type": "0x0"
  },
  "id": 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_gettransactionreceipt)
