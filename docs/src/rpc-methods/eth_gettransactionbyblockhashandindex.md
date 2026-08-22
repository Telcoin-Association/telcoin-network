---
description: >-
  Returns information about a transaction by block hash and transaction index
  position.
---

# eth\_getTransactionByBlockHashAndIndex

#### Parameters

`DATA`, 32 Bytes - Hash of a block.

`QUANTITY` - Hexadecimal of the transaction index position.

#### Returns

`Object` - A transaction object, or `null` when no transaction was found:

* `blockHash`: `DATA`, 32 Bytes - Hash of the block where this transaction was in. `null` when its pending.
* `blockNumber`: `QUANTITY` - Hexadecimal block number where this transaction was in. `null` when its pending.
* `from`: `DATA`, 20 Bytes - Address of the sender.
* `gas`: `QUANTITY` - Hexadecimal gas provided by the sender.
* `gasPrice`: `QUANTITY` - Hexadecimal gas price provided by the sender in Wei.
* `hash`: `DATA`, 32 Bytes - Hash of the transaction.
* `input`: `DATA` - The data send along with the transaction.
* `nonce`: `QUANTITY` - The number of transactions made by the sender prior to this one.
* `to`: `DATA`, 20 Bytes - Address of the receiver. `null` when its a contract creation transaction.
* `transactionIndex`: `QUANTITY` - Hexadecimal of the transactions index position in the block. `null` when its pending.
* `value`: `QUANTITY` - Value transferred in Wei.
* `v`: `QUANTITY` - ECDSA recovery id
* `r`: `QUANTITY` - ECDSA signature r
* `s`: `QUANTITY` - ECDSA signature s

#### Example

#### Request

<pre><code><strong>curl https://rpc.adiri.tel \
</strong> -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getTransactionByBlockHashAndIndex","params":[
        "0x3874ed915e305246b75c0d388827f2d23df451ac901c07618154bcf03e5ac519",
        "0x0"]
        ,"id":1}'
</code></pre>

#### Result

```
{
  "jsonrpc": "2.0",
  "result": {
    "hash": "0xadbfc093b90e69b6fdf3a843f03ef65c5c247e0593d3853b53d37c825e81339a",
    "nonce": "0x2bd",
    "blockHash": "0x3874ed915e305246b75c0d388827f2d23df451ac901c07618154bcf03e5ac519",
    "blockNumber": "0x608225",
    "transactionIndex": "0x0",
    "from": "0x3da87b1c3743bd2da60df2ef1bc6f26ef9da6086",
    "to": "0x5eb15fedcca9cb9c5424167e329fd56905ffe0e3",
    "value": "0x8203404a3db680000",
    "gasPrice": "0xb2d05e07",
    "gas": "0x29810",
    "input": "0x7ff36ab5000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800000000000000000000000003da87b1c3743bd2da60df2ef1bc6f26ef9da608600000000000000000000000000000000000000000000000000000000667b16b7000000000000000000000000000000000000000000000000000000000000000200000000000000000000000080d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5000000000000000000000000a2c07c15173c183771ffad40c2e972f97e0bce64",
    "r": "0xd527419716bc6eb25e74420a7a28e6f3793ead979c8ca6ed8b1b27245470c83d",
    "s": "0x6bef1d101db08d829080d43b823a4896e5971f5794aaef508ea2cffea52ef9c1",
    "v": "0xfe5",
    "chainId": "0x7e1",
    "type": "0x0"
  },
  "id": 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_gettransactionbyblockhashandindex)





