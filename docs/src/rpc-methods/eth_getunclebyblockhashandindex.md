---
description: Returns information about a uncle of a block by hash and uncle index position.
---

# eth\_getUncleByBlockHashAndIndex

#### Parameters

`DATA`, 32 Bytes - Hash of a block.

`QUANTITY` - Hexadecimal of the uncle's index position.

#### Returns

`Object` - A block object, or `null` when no block was found:

* `number`: `QUANTITY` - Hexadecimal of the block number. `null` when its pending block.
* `hash`: `DATA`, 32 Bytes - Hash of the block. `null` when its pending block.
* `parentHash`: `DATA`, 32 Bytes - Hash of the parent block.
* `nonce`: `DATA`, 8 Bytes - Hash of the generated proof-of-work. `null` when its pending block.
* `sha3Uncles`: `DATA`, 32 Bytes - SHA3 of the uncles data in the block.
* `logsBloom`: `DATA`, 256 Bytes - The bloom filter for the logs of the block. `null` when its pending block.
* `transactionsRoot`: `DATA`, 32 Bytes - The root of the transaction trie of the block.
* `stateRoot`: `DATA`, 32 Bytes - The root of the final state trie of the block.
* `receiptsRoot`: `DATA`, 32 Bytes - The root of the receipts trie of the block.
* `miner`: `DATA`, 20 Bytes - The address of the beneficiary to whom the mining rewards were given.
* `difficulty`: `QUANTITY` - Hexadecimal of the difficulty for this block.
* `totalDifficulty`: `QUANTITY` - Hexadecimal of the total difficulty of the chain until this block.
* `extraData`: `DATA` - The "extra data" field of this block.
* `size`: `QUANTITY` - Hexadecimal size of this block in bytes.
* `gasLimit`: `QUANTITY` - Hexadecimal of the maximum gas allowed in this block.
* `gasUsed`: `QUANTITY` - Hexadecimal of the total used gas by all transactions in this block.
* `timestamp`: `QUANTITY` - Hexadecimal of the unix timestamp for when the block was collated.
* `transactions`: `Array` - Array of transaction objects, or 32 Bytes transaction hashes depending on the last given parameter.
* `uncles`: `Array` - Array of uncle hashes.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getUncleByBlockHashAndIndex","params":["0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb", "0x0"],"id":1}'
```

#### Result

```
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getunclebyblockhashandindex)
