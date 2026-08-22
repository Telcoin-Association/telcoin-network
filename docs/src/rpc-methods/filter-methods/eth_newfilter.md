# eth\_newFilter

Creates a filter object, based on filter options, to notify when the state changes (logs). To check if the state has changed, call [eth\_getFilterChanges](eth\_getfilterchanges.md).

**A note on specifying topic filters:** Topics are order-dependent. A transaction with a log with topics \[A, B] will be matched by the following topic filters:

* `[]` "anything"
* `[A]` "A in first position (and anything after)"
* `[null, B]` "anything in first position AND B in second position (and anything after)"
* `[A, B]` "A in first position AND B in second position (and anything after)"
* `[[A, B], [A, B]]` "(A OR B) in first position AND (A OR B) in second position (and anything after)"

#### Parameters

`Object` - The filter options:

* `fromBlock`: `QUANTITY|TAG` - (optional, default: `"latest"`) Hexadecimal of a block number, or `"latest"` for the last mined block, `"safe"` for the latest safe block, `"finalized"` for the latest finalized block, or  `"earliest"` for not yet mined transactions.
* `toBlock`: `QUANTITY|TAG` - (optional, default: `"latest"`) Hexadecimal of a block number, or `"latest"` for the last mined block, `"safe"` for the latest safe block, `"finalized"` for the latest finalized block, or  `"earliest"` for not yet mined transactions.
* `address`: `DATA|Array`, 20 Bytes - (optional) Contract address or a list of addresses from which logs should originate.
* `topics`: `Array of DATA`, - (optional) Array of 32 Bytes `DATA` topics. Topics are order-dependent. Each topic can also be an array of DATA with "or" options.

#### Returns

`DATA` - A filter id.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{
      "jsonrpc": "2.0","method": "eth_newFilter","params": [
        {
          "fromBlock": "0x60387D",
          "toBlock": "0x60387E",
          "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
          ]
        }
      ],
      "id": 1
    }'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": "0x26c221936b2025a44f60294273d7bbaa",
  "id": 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_newfilter)
