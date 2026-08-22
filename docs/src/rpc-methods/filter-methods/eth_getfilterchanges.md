# eth\_getFilterChanges

Polling method for a filter, which returns an array of logs which occurred since last poll. Filter must be created by calling either [eth\_newFilter](eth\_newfilter.md) or [eth\_newBlockFilter](eth\_newblockfilter.md).

#### Parameters

`QUANTITY` - the filter id.

#### Returns

`Array` of `DATA`  - Array of log objects, or an empty array if nothing has changed since last poll.

For filters created with `eth_newBlockFilter` the return are block hashes (`DATA`, 32 Bytes), e.g. `["0x3454645634534..."]`.

For filters created with `eth_newPendingTransactionFilter` the return are transaction hashes (`DATA`, 32 Bytes), e.g. `["0x6345343454645..."]`.

For filters created with `eth_newFilter` logs are objects with following params:

* `removed`: `TAG` - `true` when the log was removed, due to a chain reorganization. `false` if its a valid log.
* `logIndex`: `QUANTITY` - Hexadecimal of the log index position in the block. `null` when its pending log.
* `transactionIndex`: `QUANTITY` - Hexadecimal of the transaction's index position log was created from. `null` when its pending log.
* `transactionHash`: `DATA`, 32 Bytes - Hash of the transactions this log was created from. `null` when its pending log.
* `blockHash`: `DATA`, 32 Bytes - Hash of the block where this log was in. `null` when its pending. `null` when its pending log.
* `blockNumber`: `QUANTITY` - Hexadecimal of the block number this log was in. `null` when its pending. `null` when its pending log.
* `address`: `DATA`, 20 Bytes - Address from which this log originated.
* `data`: `DATA` - Contains zero or more 32 Bytes non-indexed arguments of the log.
* `topics`: `Array of DATA` - Array of 0 to 4 32 Bytes `DATA` of indexed log arguments. (In _solidity_: The first topic is the _hash_ of the signature of the event (e.g. `Deposit(address,bytes32,uint256)`), except you declared the event with the `anonymous` specifier.)

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getFilterChanges","params":["0x26c221936b2025a44f60294273d7bbaa"],"id":73}'
```

#### Result

```
{
"jsonrpc":"2.0",
"result":[
    {"address":"0x80d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5",
    "topics":[
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3"],
    "data":"0x000000000000000000000000000000000000000000000002f1cd8944b8700000",
    "blockHash":"0x59380d589f6d0f833518978956b81aa5248a497a73fb8ea1bf8a08b6abc6656b",
    "blockNumber":"0x60387d",
    "transactionHash":"0x0c52053dbf5ad86686ac00cc3858726967c33ab5eddaa889d306663f8610d659",
    "transactionIndex":"0x0",
    "logIndex":"0x0",
    "removed":false},
    {"address":
    "0x80d1f6dafc9c13e9d19aedf75e3c1e2586d4a2a5",
    "topics":[
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000005eb15fedcca9cb9c5424167e329fd56905ffe0e3",
        "0x000000000000000000000000e2005c7718f3849f39c65b8ea93c7a7030aec463"],
    "data":"0x000000000000000000000000000000000000000000000002f1cd8944b8700000",
    "blockHash":"0x59380d589f6d0f833518978956b81aa5248a497a73fb8ea1bf8a08b6abc6656b",
    "blockNumber":"0x60387d",
    "transactionHash":"0x0c52053dbf5ad86686ac00cc3858726967c33ab5eddaa889d306663f8610d659",
    "transactionIndex":"0x0",
    "logIndex":"0x1",
    "removed":false},
    {"address":"0xa2c07c15173c183771ffad40c2e972f97e0bce64",
    "topics":[
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x000000000000000000000000e2005c7718f3849f39c65b8ea93c7a7030aec463",
        "0x0000000000000000000000003da87b1c3743bd2da60df2ef1bc6f26ef9da6086"],
    "data":"0x00000000000000000000000000000000000000000000000001f9b3795d77f3a5",
    "blockHash":"0x59380d589f6d0f833518978956b81aa5248a497a73fb8ea1bf8a08b6abc6656b",
    "blockNumber":"0x60387d",
    "transactionHash":"0x0c52053dbf5ad86686ac00cc3858726967c33ab5eddaa889d306663f8610d659",
    "transactionIndex":"0x0",
    "logIndex":"0x2",
    "removed":false}],
    "id":73
    }
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getfilterchanges)
