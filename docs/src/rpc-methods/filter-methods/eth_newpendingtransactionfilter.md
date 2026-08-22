# eth\_newPendingTransactionFilter

Creates a filter in the node, to notify when new pending transactions arrive. To check if the state has changed, call [eth\_getFilterChanges](eth\_getfilterchanges.md).

#### Parameters

`None`

#### Returns

`DATA` - A filter id.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_newPendingTransactionFilter","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": "0x38d257a1aab91606194ba7211d4ce26",
  "id": 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_newpendingtransactionfilter)
