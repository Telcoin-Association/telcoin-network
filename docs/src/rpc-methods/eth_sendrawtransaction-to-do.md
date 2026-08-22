---
description: >-
  Creates new message call transaction or a contract creation for signed
  transactions.
---

# eth\_sendRawTransaction (to do)

#### Parameters

`DATA`, The signed transaction data.

#### Returns

`DATA`, 32 Bytes - The transaction hash, or the zero hash if the transaction is not yet available.

Use [eth\_getTransactionReceipt](eth\_gettransactionreceipt.md) to get the contract address, after the transaction was mined, when you created a contract.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675",],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0xe670ec64341771606e55d6b4ca35a1a6b75ee3d5145a99d05921026d1527331"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_sendrawtransaction)
