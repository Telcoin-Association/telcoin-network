# eth\_uninstallFilter

Uninstalls a filter with given id. Should always be called when watch is no longer needed. Additionally Filters timeout when they aren't requested with [eth\_getFilterChanges](eth\_getfilterchanges.md) for a period of time.

#### Parameters

`DATA` - The filter id.

#### Returns

`Boolean` - `true` if the filter was successfully uninstalled, otherwise `false`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_uninstallFilter","params":["0x26c221936b2025a44f60294273d7bbaa"],"id":73}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": true,
  "id": 73
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_uninstallfilter)
