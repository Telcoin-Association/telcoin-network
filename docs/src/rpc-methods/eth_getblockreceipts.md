# eth\_getBlockReceipts

#### Parameters

`blockNumber`: `QUANTITY|TAG` \[_Required_] - Hexadecimal block number, or one of the string tags: `"latest"` ,  `"earliest"`, `"finalized"`, `"safe".`

#### Returns

`result`: `Array` - Array of receipt objects for every transaction in the block, or `null` when there is no corresponding block. Each receipt has the following fields:

* `transactionHash`: `DATA`, 32 Bytes - Hash of the transaction.
* `transactionIndex`: `QUANTITY` - Hexadecimal of the transactions index position in the block.
* `blockHash`: `DATA`, 32 Bytes - Hash of the block where this transaction was in.
* `blockNumber`: `QUANTITY` - Hexadecimal block number where this transaction was in.
* `from`: `DATA`, 20 Bytes - Address of the sender.
* `to`: `DATA`, 20 Bytes - Address of the receiver. `null` when its a contract creation transaction.
* `cumulativeGasUsed`: `QUANTITY` - Hexadecimal of the total amount of gas used when this transaction was executed in the block.
* `effectiveGasPrice`: `QUANTITY` - Hexadecimal of the sum of the base fee and tip paid per unit of gas.
* `gasUsed`: `QUANTITY` - Hexadecimal of the amount of gas used by this specific transaction alone.
* `contractAddress`: `DATA`, 20 Bytes - The contract address created, if the transaction was a contract creation, otherwise `null`.
* `logs`: `Array` - Array of log objects, which this transaction generated.
* `logsBloom`: `DATA`, 256 Bytes - Bloom filter for light clients to quickly retrieve related logs.
* `status`: `QUANTITY` - Either `0x1` (success) or `0x0` (failure).
* `type`: `QUANTITY` - Hexadecimal of the transaction type, `0x0` for legacy transactions, `0x1` for access list types, `0x2` for dynamic fees.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getBlockReceipts","params":["0x3FE51"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": [
    {
      "transactionHash": "0x82756da4e315a0135c3a10bf056af829e9c0270c3fd2127791dd2498cf577111",
      "transactionIndex": "0x0",
      "blockHash": "0x8e69cf10fe1b0dbb7213406f58ea99904b685532d7c3604d3e1be9684bda6d4f",
      "blockNumber": "0x3fe51",
      "cumulativeGasUsed": "0xe9994",
      "gasUsed": "0xe9994",
      "effectiveGasPrice": "0x17bfac7c00",
      "from": "0xdec366b889a53b93cfa561076c03c18b0b4d6c93",
      "to": null,
      "contractAddress": "0xf9d26a8da3fccf2c65ab6ee19bf35aa08a0326d6",
      "logs": [
        {
          "address": "0xf9d26a8da3fccf2c65ab6ee19bf35aa08a0326d6",
          "topics": [
            "0xc7f505b2f371ae2175ee4913f4499e1f2633a7b5936321eed1cdaeb6115181d2"
          ],
          "data": "0x000000000000000000000000000000000000000000000000ffffffffffffffff",
          "blockHash": "0x8e69cf10fe1b0dbb7213406f58ea99904b685532d7c3604d3e1be9684bda6d4f",
          "blockNumber": "0x3fe51",
          "transactionHash": "0x82756da4e315a0135c3a10bf056af829e9c0270c3fd2127791dd2498cf577111",
          "transactionIndex": "0x0",
          "logIndex": "0x0",
          "removed": false
        }
      ],
      "logsBloom": "0x00000000000000000000000000000000000000000000006000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000000000000000000000080000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "status": "0x1",
      "type": "0x0"
    }
  ],
  "id": 1
}
```

[source](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getblockreceipts)
